from __future__ import annotations

from pydantic import BaseModel

from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.enums import EventType
from agent_hub.store import projections


class RollbackReport(BaseModel):
    checkpoint_id: str
    plan_version: int
    reset_node_ids: list[str]
    cancelled_remote_task_ids: list[str]
    mode: str


async def plan_rollback(db, task_id: str, checkpoint_id: str) -> RollbackReport:
    task = await projections.fetch_task(db, task_id)
    checkpoint = await projections.fetch_checkpoint(db, checkpoint_id)
    if task is None or checkpoint is None or checkpoint.task_id != task_id:
        raise TaskNotFound(f"task/checkpoint not found: {task_id}/{checkpoint_id}")
    plan = await projections.fetch_current_plan(db, task_id)
    if plan is None:
        raise TaskNotFound(f"plan not found: {task_id}")
    nodes = await projections.fetch_nodes(db, task_id, plan.id)
    frontier = set(checkpoint.frontier)
    reset = [node for node in nodes if node.id not in frontier]
    return RollbackReport(
        checkpoint_id=checkpoint_id,
        plan_version=checkpoint.plan_version,
        reset_node_ids=[node.id for node in reset],
        cancelled_remote_task_ids=[
            node.a2a_task_id for node in reset if node.a2a_task_id
        ],
        mode="dry_run",
    )


async def perform_rollback(
    db, events, remote, orchestrator, task_id: str, checkpoint_id: str
) -> RollbackReport:
    report = await plan_rollback(db, task_id, checkpoint_id)
    report = report.model_copy(update={"mode": "restart"})
    for node_id in report.reset_node_ids:
        node = await projections.fetch_node(db, node_id)
        if node is None or not node.a2a_task_id:
            continue
        await remote.cancel_task(node.agent_url or "", node.a2a_task_id)
        await events.append(
            task_id,
            EventType.NODE_CANCEL_SENT,
            {"node_id": node.id, "a2a_task_id": node.a2a_task_id},
        )
    await events.append(
        task_id,
        EventType.ROLLBACK_PERFORMED,
        {
            "checkpoint_id": checkpoint_id,
            "plan_version": report.plan_version,
            "reset_node_ids": report.reset_node_ids,
        },
    )
    orchestrator.start(task_id)
    return report
