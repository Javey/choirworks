from __future__ import annotations

from typing import Any

from choirworks.core.tasks import TaskNotFound
from choirworks.models.enums import EventType, NodeStatus, TaskStatus
from choirworks.store import projections


class TaskNotCancelable(ValueError):
    pass


CANCELLABLE_TASK_STATUSES = (
    TaskStatus.RUNNING,
    TaskStatus.AWAITING_INPUT,
    TaskStatus.PLANNING,
)


async def cancel_task(
    db: Any,
    events: Any,
    remote: Any,
    orchestrator: Any,
    task_id: str,
) -> None:
    task = await projections.fetch_task(db, task_id)
    if task is None:
        raise TaskNotFound(task_id)
    if task.status not in CANCELLABLE_TASK_STATUSES:
        raise TaskNotCancelable(f"task is {task.status.value}")

    plan = await projections.fetch_current_plan(db, task_id)
    if plan is not None:
        for node in await projections.fetch_nodes(db, task_id, plan.id):
            if node.a2a_task_id and node.status in (
                NodeStatus.DISPATCHED,
                NodeStatus.WORKING,
                NodeStatus.INPUT_REQUIRED,
            ):
                await remote.cancel_task(node.agent_url or "", node.a2a_task_id)
                await events.append(
                    task_id,
                    EventType.NODE_CANCEL_SENT,
                    {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
                )
    await events.append(
        task_id,
        EventType.TASK_STATE_CHANGED,
        {"from": task.status.value, "to": TaskStatus.CANCELED.value},
    )
    await orchestrator.stop_task(task_id)
