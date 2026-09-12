from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import CreateTaskIn
from agent_hub.core.dispatcher import InvalidNodeState
from agent_hub.core.tasks import (
    CreatedTask,
    TargetSpec,
    TaskNotFound,
    TaskSnapshot,
    UnknownAgent,
)

router = APIRouter(tags=["tasks"])


@router.post("/tasks", status_code=201, response_model=CreatedTask)
async def create_task(body: CreateTaskIn, request: Request) -> CreatedTask:
    service = request.app.state.task_service
    target = TargetSpec(
        agent_name=body.target.agent_name,
        skill_id=body.target.skill_id,
        name=body.target.name,
        input=body.target.input,
    )
    try:
        return await service.create_task(body.request, target)
    except UnknownAgent as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tasks/{task_id}", response_model=TaskSnapshot)
async def get_task(task_id: str, request: Request) -> TaskSnapshot:
    try:
        return await request.app.state.task_service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc


@router.post("/tasks/{task_id}/nodes/{node_id}/dispatch", response_model=TaskSnapshot)
async def dispatch_node(task_id: str, node_id: str, request: Request) -> TaskSnapshot:
    dispatcher = request.app.state.dispatcher
    service = request.app.state.task_service
    try:
        await dispatcher.dispatch_node(task_id, node_id)
    except InvalidNodeState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await service.finalize_if_complete(task_id)
    return await service.get_snapshot(task_id)
