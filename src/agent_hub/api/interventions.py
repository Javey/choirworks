from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from agent_hub.api.schemas import AnswerInterventionIn
from agent_hub.core.dispatcher import InvalidNodeState
from agent_hub.core.tasks import TaskNotFound
from agent_hub.models.domain import Intervention
from agent_hub.models.enums import InterventionStatus
from agent_hub.store import projections

router = APIRouter(tags=["interventions"])


@router.get("/tasks/{task_id}/interventions", response_model=list[Intervention])
async def list_interventions(
    task_id: str, request: Request, status: InterventionStatus | None = None
) -> list[Intervention]:
    try:
        await request.app.state.task_service.get_snapshot(task_id)
    except TaskNotFound as exc:
        raise HTTPException(status_code=404, detail=f"task not found: {task_id}") from exc
    return await projections.fetch_interventions(request.app.state.db, task_id, status)


@router.post(
    "/tasks/{task_id}/interventions/{intervention_id}",
    response_model=Intervention,
)
async def answer_intervention(
    task_id: str,
    intervention_id: str,
    body: AnswerInterventionIn,
    request: Request,
) -> Intervention:
    intervention = await projections.fetch_intervention(
        request.app.state.db, intervention_id
    )
    if intervention is None or intervention.task_id != task_id:
        raise HTTPException(
            status_code=404, detail=f"intervention not found: {intervention_id}"
        )
    try:
        await request.app.state.orchestrator.answer_intervention(
            intervention_id, body.text, responder=body.responder
        )
    except InvalidNodeState as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    refreshed = await projections.fetch_intervention(
        request.app.state.db, intervention_id
    )
    assert refreshed is not None
    return refreshed
