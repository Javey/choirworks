from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from choirworks.api.schemas import RollbackIn
from choirworks.core.cancel import TaskNotCancelable
from choirworks.core.cancel import cancel_task as core_cancel_task
from choirworks.core.rollback import RollbackReport, perform_rollback, plan_rollback
from choirworks.core.tasks import TaskNotFound, TaskSnapshot

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
    try:
        await core_cancel_task(
            request.app.state.db,
            request.app.state.event_store,
            request.app.state.remote,
            request.app.state.orchestrator,
            task_id,
        )
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc
    except TaskNotCancelable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return await request.app.state.task_service.get_snapshot(task_id)
