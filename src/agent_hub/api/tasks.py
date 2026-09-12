from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import CreateTaskIn, CreateTaskOut
from agent_hub.core.dispatcher import InvalidNodeState
from agent_hub.core.tasks import (
    TargetSpec,
    TaskNotFound,
    TaskSnapshot,
    UnknownAgent,
)

router = APIRouter(tags=["tasks"])


@router.post("/tasks", status_code=201, response_model=CreateTaskOut)
async def create_task(body: CreateTaskIn, request: Request) -> CreateTaskOut:
    service = request.app.state.task_service
    if body.target is not None:
        target = TargetSpec(
            agent_name=body.target.agent_name,
            skill_id=body.target.skill_id,
            name=body.target.name,
            input=body.target.input,
        )
        try:
            created = await service.create_task(body.request, target)
        except UnknownAgent as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return CreateTaskOut(
            task_id=created.task_id,
            plan_id=created.plan_id,
            node_ids=created.node_ids,
        )
    task_id = await service.create_pending_task(body.request)
    request.app.state.orchestrator.start(task_id)
    return CreateTaskOut(task_id=task_id)


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
