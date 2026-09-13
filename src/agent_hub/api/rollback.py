from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import RollbackIn
from agent_hub.core.rollback import RollbackReport, perform_rollback, plan_rollback
from agent_hub.core.tasks import TaskNotFound, TaskSnapshot
from agent_hub.models.enums import EventType, NodeStatus, TaskStatus
from agent_hub.store import projections

router = APIRouter(tags=["rollback"])


@router.post("/tasks/{task_id}/rollback", response_model=RollbackReport)
async def rollback(task_id: str, body: RollbackIn, request: Request) -> RollbackReport:
    try:
        if body.mode == "dry_run":
            return await plan_rollback(
                request.app.state.db,
                request.app.state.event_store,
                task_id,
                body.checkpoint_id,
            )
        return await perform_rollback(
            request.app.state.db,
            request.app.state.event_store,
            request.app.state.remote,
            request.app.state.orchestrator,
            task_id,
            body.checkpoint_id,
        )
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/tasks/{task_id}/nodes/{node_id}/retry", response_model=TaskSnapshot
)
async def retry_node(task_id: str, node_id: str, request: Request) -> TaskSnapshot:
    try:
        await request.app.state.task_service.retry_node(task_id, node_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    request.app.state.orchestrator.start(task_id)
    return await request.app.state.task_service.get_snapshot(task_id)


@router.post("/tasks/{task_id}/cancel", response_model=TaskSnapshot)
async def cancel_task(task_id: str, request: Request) -> TaskSnapshot:
    db = request.app.state.db
    task = await projections.fetch_task(db, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}")
    cancellable = (
        TaskStatus.RUNNING,
        TaskStatus.AWAITING_INPUT,
        TaskStatus.PLANNING,
    )
    if task.status not in cancellable:
        raise HTTPException(status_code=409, detail=f"task is {task.status.value}")

    events = request.app.state.event_store
    plan = await projections.fetch_current_plan(db, task_id)
    if plan is not None:
        for node in await projections.fetch_nodes(db, task_id, plan.id):
            if node.a2a_task_id and node.status in (
                NodeStatus.DISPATCHED,
                NodeStatus.WORKING,
                NodeStatus.INPUT_REQUIRED,
            ):
                await request.app.state.remote.cancel_task(
                    node.agent_url or "", node.a2a_task_id
                )
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
    await request.app.state.orchestrator.stop_task(task_id)
    return await request.app.state.task_service.get_snapshot(task_id)
